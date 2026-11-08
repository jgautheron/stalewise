#!/usr/bin/env bash
set -euo pipefail
operation=$1
namespace=$2
fixtures="$STALEWISE_ROOT/test/chainsaw/controller"

install() {
  sed "s|STALEWISE_TEST_IMAGE|$STALEWISE_TEST_IMAGE|g" "$fixtures/origin.yaml" | kubectl -n "$namespace" apply -f -
  if ! kubectl -n "$namespace" rollout status deployment/origin --timeout=90s; then
    kubectl -n "$namespace" describe pods
    kubectl -n "$namespace" logs deployment/origin --all-containers
    return 1
  fi
  kubectl -n "$namespace" create secret generic mesh-token --from-literal=token=integration-token-0123456789abcdef
  for release in cache mesh shard; do
    values=()
    if [[ $release == mesh ]]; then
      values=(-f "$STALEWISE_ROOT/charts/stalewise/values-mesh.yaml"
        --set "backend.host=catalog.$namespace.svc.cluster.local"
        --set "canonicalHost=catalog.$namespace.svc.cluster.local"
        --set signaller.tokenSecret.name=mesh-token --set mesh.peerAuthentication=true
        --set cachePolicies.enabled=false)
    fi
    if [[ $release == shard ]]; then values=(--set shard.enabled=true); fi
    helm upgrade --install "$release" "$STALEWISE_ROOT/charts/stalewise" -n "$namespace" \
      "${values[@]}" --set fullnameOverride="$release" --set backend.service=catalog \
      --set image.repository="${STALEWISE_TEST_IMAGE%:*}" --set image.tag="${STALEWISE_TEST_IMAGE##*:}" --set image.pullPolicy=Never \
      --set 'storage=malloc\,16m' --set cachePolicies.enabled="$([[ $release == cache ]] && echo true || echo false)" \
      --wait --timeout 180s
  done
  for release in cache mesh shard; do
    kubectl -n "$namespace" wait pod -l "app.kubernetes.io/instance=$release" --for=condition=Ready --timeout=90s
    status=$(kubectl -n "$namespace" exec deployment/origin -- curl -sS --max-time 5 -o /dev/null -w '%{http_code}' "http://$release-management:9090/readyz")
    [[ $status == 200 ]]
    body=$(kubectl -n "$namespace" exec deployment/origin -- curl -fsS --max-time 5 "http://$release:8080/")
    [[ $body == 'origin fixture' ]]
  done
}

policies() {
  sed 's/ttlSeconds: 60/ttlSeconds: -1/; s/name: catalog$/name: invalid/' "$fixtures/policy.yaml" > "$STALEWISE_TEST_TMP/invalid.yaml"
  if kubectl -n "$namespace" apply -f "$STALEWISE_TEST_TMP/invalid.yaml" > "$STALEWISE_TEST_TMP/admission.log" 2>&1; then
    echo 'API server accepted a negative TTL' >&2; return 1
  fi
  grep -Fq 'spec.overrides.ttlSeconds' "$STALEWISE_TEST_TMP/admission.log"
  [[ $(kubectl -n "$namespace" get cachepolicy invalid --ignore-not-found -o name) == '' ]]
  for n in {1..10}; do
    cat <<YAML | kubectl -n "$namespace" apply -f -
apiVersion: stalewise.dev/v1alpha1
kind: CachePolicy
metadata: {name: many-$n}
spec:
  serviceRef: {name: catalog}
  pathPrefix: /many/$n
  overrides: {ttlSeconds: 60}
YAML
  done
  for n in {1..10}; do
    kubectl -n "$namespace" wait "cachepolicy/many-$n" --for=condition=Accepted --timeout=120s
  done
  for verb in create update patch delete; do
    result=$(kubectl -n "$namespace" auth can-i "$verb" virtualservices.networking.istio.io \
      --as="system:serviceaccount:$namespace:cache" || true)
    [[ $result == no ]]
  done
  result=$(kubectl -n "$namespace" auth can-i list virtualservices.networking.istio.io \
    --as="system:serviceaccount:$namespace:cache")
  [[ $result == yes ]]
}

rollout() {
  kubectl -n "$namespace" rollout restart deployment/cache
  kubectl -n "$namespace" rollout status deployment/cache --timeout=180s > "$STALEWISE_TEST_TMP/rollout.log" 2>&1 &
  waiter=$!
  trap 'kill "$waiter" 2>/dev/null || true; wait "$waiter" 2>/dev/null || true' EXIT
  for n in {1..40}; do
    body=$(kubectl -n "$namespace" exec deployment/origin -- curl -fsS --max-time 5 http://cache:8080/)
    [[ $body == 'origin fixture' ]]
    if ! kill -0 "$waiter" 2>/dev/null; then break; fi
    sleep 0.25
  done
  wait "$waiter"
  kubectl -n "$namespace" get pdb cache -o jsonpath='{.spec.maxUnavailable}' | grep -qx 1
}

case "$operation" in
  install) install ;;
  break-attachment) kubectl -n "$namespace" patch virtualservice catalog --type=json \
    -p='[{"op":"remove","path":"/spec/http/0"}]' ;;
  conflict) sed 's/name: catalog$/name: conflicting/' "$fixtures/policy.yaml" | kubectl -n "$namespace" apply -f - ;;
  policies) policies ;;
  rollout) rollout ;;
  *) echo "Unknown check: $operation" >&2; exit 1 ;;
esac
