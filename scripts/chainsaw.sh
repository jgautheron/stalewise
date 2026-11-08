#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
export STALEWISE_ROOT=$PWD
export STALEWISE_TEST_IMAGE=${STALEWISE_TEST_IMAGE:-stalewise:tests}
export STALEWISE_TEST_TMP
STALEWISE_TEST_TMP=$(mktemp -d)
export KUBECONFIG="$STALEWISE_TEST_TMP/kubeconfig"
cluster="stalewise-tests-$$"
created=false
cleanup() {
  if $created; then .tools/kind delete cluster --name "$cluster"; fi
  rm -rf "$STALEWISE_TEST_TMP"
}
trap cleanup EXIT
mkdir -p .tools
case "$(uname -s)-$(uname -m)" in
  Linux-x86_64)
    platform=linux_amd64
    chainsaw_sha=295d226c89f126c0a97775d364be149f47a810c8a3f9829ee410583d0c1abe3c
    kind_sha=eb244cbafcc157dff60cf68693c14c9a75c4e6e6fedaf9cd71c58117cb93e3fa ;;
  Darwin-arm64)
    platform=darwin_arm64
    chainsaw_sha=502bef17dd822217e82590e9ccf01c1e26a56b3fa34a835789b9af73e82a169a
    kind_sha=88bf554fe9da6311c9f8c2d082613c002911a476f6b5090e9420b35d84e70c5c ;;
  *) echo 'Supported hosts: Linux x86_64 and macOS arm64' >&2; exit 1 ;;
esac
curl -fsSL --retry 3 --max-time 120 "https://github.com/kyverno/chainsaw/releases/download/v0.2.15/chainsaw_$platform.tar.gz" -o "$STALEWISE_TEST_TMP/chainsaw.tar.gz"
echo "$chainsaw_sha  $STALEWISE_TEST_TMP/chainsaw.tar.gz" | shasum -a 256 -c -
tar -xzf "$STALEWISE_TEST_TMP/chainsaw.tar.gz" -C .tools chainsaw
curl -fsSL --retry 3 --max-time 120 "https://github.com/kubernetes-sigs/kind/releases/download/v0.31.0/kind-${platform/_/-}" -o .tools/kind
echo "$kind_sha  .tools/kind" | shasum -a 256 -c -
chmod +x .tools/kind
if .tools/kind get clusters | grep -qx "$cluster"; then echo 'Refusing to reuse an existing cluster' >&2; exit 1; fi
created=true
.tools/kind create cluster --name "$cluster" --wait 180s \
  --image kindest/node:v1.35.0@sha256:452d707d4862f52530247495d180205e029056831160e22870e37e3f6c1ac31f
.tools/kind load docker-image "$STALEWISE_TEST_IMAGE" --name "$cluster"
echo 'c39b1abd1c3485809fa77441266f4ebb53d9305643c0e754db51fe0d1f11c354  test/chainsaw/vendor/istio-crds.yaml' | shasum -a 256 -c -
kubectl apply --server-side -f test/chainsaw/vendor/istio-crds.yaml
kubectl wait --for=condition=Established --timeout=60s \
  crd/virtualservices.networking.istio.io crd/authorizationpolicies.security.istio.io crd/peerauthentications.security.istio.io
report_dir=${CHAINSAW_REPORT_DIR:-$STALEWISE_ROOT/.tools/reports/$cluster}
mkdir -p "$report_dir"
.tools/chainsaw test "${@:-test/chainsaw}" --remarshal --cleanup-timeout 120s --delete-timeout 120s --exec-timeout 240s --assert-timeout "${CHAINSAW_ASSERT_TIMEOUT:-120s}" \
  --report-format JUNIT-STEP --report-path "$report_dir"
