# 0006: Cluster-installed operator, application-owned cache requests

## Context

Application charts should request caches without embedding workload packaging or coordinating separate releases.
A platform team installs the operator once per cluster.
This extends standalone installation; it does not replace supervision or ADR 0003 route ownership.

## Decision

### Resource contract

Use namespaced `stalewise.dev/v1alpha1`, kind `HTTPCache`, plural `httpcaches`.
The name distinguishes provisioned infrastructure from `CachePolicy`, which remains separate and application-owned.
The operator never adopts or deletes policies.

This example shows the complete normal spec, including optional overrides:

```yaml
apiVersion: stalewise.dev/v1alpha1
kind: HTTPCache
metadata: {name: catalog-cache, namespace: apps}
spec:
  originServiceRef: {name: catalog, port: 8080}
  canonicalHost: catalog.apps.svc.cluster.local
  replicas: 2
  storage: malloc,256m
  transientStorage: malloc,64m
  capacity: {maxObjects: 65536}
  memguard: {enabled: true, high: 0.85, low: 0.75, minAge: 10s, interval: 1s}
  shard: {enabled: false, localTtl: 0s, membershipDebounce: 5s, peerTimeout: 2s}
  mesh: {enabled: false, proxy: {concurrency: 0, cpuRequest: 100m, cpuLimit: "1"}}
  resources: {requests: {cpu: 100m, memory: 512Mi}, limits: {memory: 512Mi}}
  cachePolicies: {enabled: false}
  signaller: {enabled: false}
  drainDelay: 10
  networkPolicy: {enabled: false, trafficFrom: [], managementFrom: [], signallerFrom: []}
```

Only `originServiceRef` is required: a same-namespace Service name and numeric TCP Service port, not its targetPort.
The operator resolves that Service and uses its DNS as a static origin in either profile.
An absent canonicalHost derives `<svc>.<ns>.svc.cluster.local`; it never changes the upstream destination.
Reject ExternalName, headless origins, missing ports, and origin/cache identity collisions.

Other defaults match the example, except `resources` and `mesh.proxy` default to empty maps.
CPU request defaults to 100m; memory limit derives from stores plus 128Mi plus 1KiB per expected object.
An absent memory request equals the limit.
`maxObjects` sizes memory; it does not cap stored objects.
Storage means bounded malloc memory, not persistent volumes.

The schema bounds strings, arrays, integers, durations and quantities; it exposes no arbitrary pod templates, arguments, volumes or secret references.
Initially accept positive `malloc,<integer>m` stores and integer Mi memory overrides, keeping sizing admission expressible in CEL.
CEL enforces the memory floor, requests ≤ limits, `0 < low < high < 1`, and minAge ≤ 24h.
It also enforces shard timing bounds and proxy concurrency ≤ CPU-limit cores, requiring a limit when concurrency is positive.
`drainDelay` is integer seconds, 0–29; shutdown remains 30s.
Names are DNS labels limited to 50 characters for suffixes.
Runtime validation handles external references and operator settings; invalid updates retain existing children.

Optional `spec.image` accepts only digest-qualified images when `--allow-image-override` is enabled.
Otherwise reconciliation rejects it with `ImageOverrideForbidden` before changing children.
CEL cannot inspect process flags; this gate is not admission rejection.
Ordinary caches follow the release's pinned cache digest, never a per-CR default pin.
The operator chart supplies this release mapping; reconciliation never queries a registry.

### Ownership and attachment

The operator creates a Deployment, three Services, ServiceAccount, Role, RoleBinding and PDB.
Traffic uses the CR name; peers and management append `-peers` and `-management`.
Ports remain 8080, 8081 when enabled, and 9090; probes use pod-only 9091.
Pod labels include `app.kubernetes.io/name=stalewise` and `app.kubernetes.io/instance=<CR-name>`.

Mesh mode injects the sidecar and creates STRICT PeerAuthentication.
Signalling or sharding additionally requires a cache-selected AuthorizationPolicy denying foreign invalidations and forged shard headers.
Only then may configuration trust sidecar loopback.
Optional NetworkPolicy retains explicit source lists: empty means deny, with peer HTTP allowed and no probe-port rule.
Egress restrictions remain a platform responsibility.

Enabling signalling creates `<name>-signaller`, an immutable random-token Secret, before starting pods.
Reuse its token across reconciliations; never rotate it during an operator upgrade.
Secret recreation changes a pod-template Secret-UID annotation and queues a rollout; callers must refresh credentials.
Inspect only metadata until ownership is verified; read token bytes only from this operator-owned Secret.
A collision never authorizes reading application credentials.
Disabling signalling removes the mount first; delete the Secret after rollout completion.

Every direct child has the HTTPCache UID as controller ownerReference; indirect pods inherit Deployment ownership.
Deletion uses garbage collection, without an operator-dependent finalizer.
Apply desired children with server-side apply, field manager `stalewise-operator`, without force.
Child watches and periodic resync repair drift; foreign ownership or field conflicts report `OwnershipConflict` without adoption.
Token data is preserved in desired Secret applies, never regenerated on resync.
Security prerequisites precede workloads; removing protections waits until dependent pods terminate.

The application chart still owns its VirtualService, including source bypass, header bypass, read routes, and unchanged normal routes, in that order.
Preserve existing path/header branches, retry rules and canary weights in both bypass groups.
The operator reuses `attach::validate`; it never writes VirtualServices or modifies origin authorization.
Validate matching CachePolicy attachment requests, or GET/HEAD on `/` when none exist.
Non-mesh mode reports `NotRequested`; it requires explicit cache DNS routing.
Unsupported matches cannot prove attachment; smoke tests remain mandatory.
Hits skip origin authorization: application charts must reject protected-route opt-in unless authorization also executes before cache access.
A blanket cache-identity ALLOW is not a solution.

### Status and failures

`status` contains `observedGeneration`, `readyReplicas`, `image`, `serviceHost`, `conditions[]`, and `attachment`.
`image` is the applied Deployment image; during rollout older replicas may still serve.
`observedGeneration` means evaluated, not successfully applied.
Attachment contains status, reason, message and checked time; preserve existing validator reasons.
Unavailable route reads yield `Unknown/VirtualServicesUnavailable`, never success from stale observations.

Conditions carry generation and transition timestamps:

- `Ready=True/Available` means the desired Deployment revision has all requested replicas available, independent of attachment.
- `Progressing=True` uses `Creating`, `Queued`, or `RollingOut`; convergence uses `False/Reconciled`.
- `Degraded=True` uses `InvalidSpec`, `OriginUnavailable`, `DependencyUnavailable`, `ImageOverrideForbidden`, `OwnershipConflict`, `ApplyFailed`, or `RolloutStalled`.

API calls have 30s deadlines; watches reconnect after 300s.
Transient failures retry with jittered 0.5–30s backoff; conflicts and invalid configuration await changes plus periodic resync.
Emit bounded Events, reason-labelled counters and sanitized logs, never token values.
Operators correct references, permissions or field ownership; stalled rollouts require repair or rollback.
An outage freezes reconciliation, not serving: caches keep their last configuration and local readiness behavior.
Status becomes stale; no ready endpoints still means failure, not automatic origin fallback.

### Upgrades and security

Every operator release changes a pod-template release revision, rolling every managed cache, including permitted image overrides.
`--max-concurrent-rollouts=1` bounds active cache Deployments, not reconcile tasks.
A single Lease-elected leader reconstructs unfinished rollouts before admitting more, including after restart.
Stalled rollouts retain slots; scale/template changes use the same admission gate.
Use maxUnavailable=0, maxSurge=1 and PDB maxUnavailable=1; the PDB alone does not constrain Deployment updates.
SIGTERM uses drainDelay; grace is at least drainDelay + 30 + 5 seconds, with no preStop delay.
Mesh shutdown exceeds drainDelay and holds startup until its proxy is ready.

Creating Roles and RoleBindings is privilege-escalation-sensitive.
Grant neither `bind` nor `escalate`: the operator holds every permission in the fixed cache Role.
Those permissions are EndpointSlice reads, optional CachePolicy reads/status writes, and optional VirtualService reads.
CachePolicy status writes use `resourceNames` for matching policies; policy changes refresh the Role without rolling pods.
The cache never receives workload-write privileges.
Native ValidatingAdmissionPolicy CEL restricts operator-written Roles and bindings to these rules and the generated cache identity.
Install this fail-closed guard before granting operator write access.
This is necessary because the operator also holds workload-write permissions; RBAC alone cannot constrain Role contents.
Do not pretend `resourceNames` can limit create requests or label-selected watches.
Scope operator Secret reads to metadata watches and verified generated-token fetches; never list Secret payloads.
The operator never mounts or reads application secrets; only generated token Secrets enter its desired state.
Its workload-write authority remains powerful: the operator is trusted platform infrastructure, not a tenant sandbox.

Require exactly one watch mode: `--watch-all-namespaces`, `--namespace-selector`, or `--namespaces`.
Never default to all namespaces.
Scope removal freezes children rather than deleting them.
Filters are not authorization boundaries; restricted installations need platform-managed namespace RoleBindings instead of a cluster-wide binding.
No admission webhook is required; schemas and CEL own declarative validation.

### Packaging, migration and rollback

Plan `charts/stalewise-operator` with both CRDs, operator Deployment, ServiceAccount, ClusterRole/bindings, Lease permissions and admission policies.
The platform owns CRD updates before binary upgrades and retains CRDs on uninstall; Helm's `crds/` directory alone does not upgrade them.
Standalone releases remain supported; designate one CRD owner and skip CRD installation in subsequent standalone installs.

First unify rendering rules, then install CRDs/operator, then create distinctly named HTTPCaches.
Wait for Ready, update application routes, and smoke-test before draining old standalone caches.
Never silently adopt existing Helm children.
Rollback restores direct routes, waits for propagation, then deletes the CR or returns traffic to retained standalone caches.
A compatible operator downgrade rolls its pinned image through the same queue; retain CRDs and reject incompatible downgrades.

## Rejected alternatives and consequences

- Subchart dependency: retains packaging and identity coupling in every application chart.
- Copied manifests: duplicates sizing and security maintenance.
- Per-service ArgoCD application: splits readiness, route ownership and rollback.
- Helm-in-operator: adds release-state ownership beside Kubernetes reconciliation.
- Default per-CR image pins: prevents centrally managed fleet upgrades.

Accept centralized upgrade blast radius and powerful platform credentials, bounded by rollout admission and namespace scoping.
