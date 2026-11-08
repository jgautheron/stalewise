#!/usr/bin/env bash
# Renders the chart for the documented release and checks names, RBAC and manifest schemas.
set -euo pipefail
chart="$(cd "$(dirname "$0")/../charts/stalewise" && pwd)"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
out="$work/rendered.yaml"

fail() { echo "chart-check: $*" >&2; exit 1; }
expect() { grep -qF -- "$1" "$out" || fail "missing: $1"; }

# kubeconform has no schema for our CRD, so derive one; Istio kinds are skipped.
# The API server rejects unknown spec fields under strict field validation; mirror that here.
yq -o=json '.spec.versions[0].schema.openAPIV3Schema
  | (.properties.spec | .. | select(type == "!!map" and has("properties"))) += {"additionalProperties": false}' \
  "$chart/crds/cachepolicies.stalewise.dev.yaml" >"$work/cachepolicy-v1alpha1.json"
validate() {
  kubeconform -strict -summary -kubernetes-version 1.33.0 -skip AuthorizationPolicy,PeerAuthentication \
    -schema-location default \
    -schema-location "$work/{{.ResourceKind}}-{{.ResourceAPIVersion}}.json" "$1"
}

helm template catalog-cache "$chart" --namespace apps -f "$chart/values-mesh.yaml" >"$out"
expect 'name: catalog-cache'
expect 'value: "catalog-cache.apps.svc.cluster.local"'
expect 'app.kubernetes.io/instance: catalog-cache'
expect 'value: "app.kubernetes.io/instance=catalog-cache,app.kubernetes.io/name=stalewise"'
expect 'resources: ["cachepolicies/status"]'
expect 'resources: ["virtualservices"]'
expect 'kind: CachePolicy'
expect 'name: STALEWISE_MAX_CACHE_POLICIES'
expect '{"concurrency":4,"holdApplicationUntilProxyStarts":true,"terminationDrainDuration":"15s"}'
expect 'sidecar.istio.io/proxyCPU: 500m'
expect 'sidecar.istio.io/proxyCPULimit: "4"'
if grep -qE 'verbs: .*"(create|delete)"' "$out"; then fail "the Role must not grant writes"; fi
validate "$out"

helm template catalog-cache "$chart" --namespace apps --set backend.service=catalog >"$out"
expect 'name: catalog-cache-stalewise'
expect 'value: "malloc,64m"'
expect 'value: "1048576"'
expect 'memory: 512Mi'
expect 'value: "10000"'
if grep -qF 'sidecar.istio.io/proxyCPU' "$out"; then fail "sidecar CPU annotations rendered without mesh.proxy"; fi
expect 'terminationGracePeriodSeconds: 45'
expect 'value: "10s"'
if grep -qF 'cachepolicies' "$out"; then fail "policy RBAC rendered while cachePolicies is disabled"; fi
if grep -qF 'kind: PeerAuthentication' "$out"; then fail "PeerAuthentication rendered by default"; fi
validate "$out"
helm template catalog-cache "$chart" --namespace apps -f "$chart/values-mesh.yaml" \
  --set shard.enabled=true --set shard.localTtl=5s >"$out"
expect 'name: STALEWISE_SHARD'
expect 'value: "5s"'
expect 'fieldPath: metadata.name'
expect 'value: "catalog-cache-peers.apps.svc.cluster.local:8080"'
expect 'key: request.headers[x-stalewise-shard-hop]'
validate "$out"

helm template catalog-cache "$chart" --namespace apps -f "$chart/values-mesh.yaml" \
  --set mesh.peerAuthentication=true >"$out"
expect 'kind: PeerAuthentication'
expect 'mode: STRICT'
validate "$out"

if helm template catalog-cache "$chart" --namespace apps --set backend.service=catalog \
  --set shard.enabled=true --set mesh.sidecar=true >/dev/null 2>&1; then
  fail "sharding behind a sidecar rendered without the AuthorizationPolicy"
fi
helm template catalog-cache "$chart" --namespace apps --set backend.service=catalog \
  --set networkPolicy.enabled=true --set signaller.enabled=true \
  --set signaller.tokenSecret.name=token >"$out"
expect 'kind: NetworkPolicy'
expect 'port: probe'
expect 'value: "$(STALEWISE_PROBE_IP):9091"'
policy="$(yq 'select(.kind == "NetworkPolicy") | .spec.ingress' "$out")"
if grep -qE 'port: (management|signaller|probe)' <<<"$policy"; then fail "empty source lists must admit nobody"; fi
[[ "$(yq 'select(.kind == "NetworkPolicy") | .spec.ingress | length' "$out")" == 1 ]] || fail "only the peer rule may render"
if [[ -n "$(yq 'select(.kind == "NetworkPolicy") | .spec.ingress[] | select(has("from") | not)' "$out")" ]]; then
  fail "an ingress rule without sources admits everyone"
fi
validate "$out"
helm template catalog-cache "$chart" --namespace apps --set backend.service=catalog \
  --set drainDelay=20 --set shutdownTimeout=60 >"$out"
expect 'terminationGracePeriodSeconds: 85'
if helm template catalog-cache "$chart" --namespace apps --set backend.service=catalog \
  --set drainDelay=30 >/dev/null 2>&1; then
  fail "drainDelay not shorter than shutdownTimeout rendered"
fi

# Memory limit: derived from storage and capacity.maxObjects, never below their minimum.
helm template catalog-cache "$chart" --namespace apps --set backend.service=catalog \
  --set 'storage=malloc\,500m' --set capacity.maxObjects=256000 >"$out"
expect 'memory: 942Mi'
# refuses <expected error> <helm args...>: rendering must fail with that message.
refuses() {
  local why="$1"; shift
  if helm template catalog-cache "$chart" --namespace apps "$@" >"$work/err" 2>&1; then fail "rendered despite: $why"; fi
  grep -qF -- "$why" "$work/err" || fail "wrong error, want '$why': $(cat "$work/err")"
}
refuses "is below the 272Mi" --set backend.service=catalog \
  --set 'storage=malloc\,64m' --set 'transientStorage=malloc\,16m' --set resources.limits.memory=192Mi
refuses "must be an integer quantity" --set backend.service=catalog --set resources.limits.memory=0.5Gi
refuses "exceeds the sidecar CPU limit of 2 cores" -f "$chart/values-mesh.yaml" --set mesh.proxy.cpuLimit=2
refuses "needs mesh.proxy.cpuLimit" -f "$chart/values-mesh.yaml" --set mesh.proxy.cpuLimit=
echo "chart-check: ok"
