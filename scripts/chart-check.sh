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
if grep -qE 'verbs: .*"(create|delete)"' "$out"; then fail "the Role must not grant writes"; fi
validate "$out"

helm template catalog-cache "$chart" --namespace apps --set backend.service=catalog >"$out"
expect 'name: catalog-cache-stalewise'
expect 'value: "malloc,64m"'
expect 'memory: 512Mi'
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
echo "chart-check: ok"
