# Istio CRDs

`istio-crds.yaml` contains only VirtualService, AuthorizationPolicy, and PeerAuthentication CRDs. No mesh components are installed.

Source: Istio 1.31.4, commit `a079f649cbcb984cd8d9330699e165b89ff1f6a1`.

Upstream URL: https://raw.githubusercontent.com/istio/istio/a079f649cbcb984cd8d9330699e165b89ff1f6a1/manifests/charts/base/files/crd-all.gen.yaml

The extraction splits the upstream file on `\n---` and retains documents with these exact `metadata.name` values:

- `virtualservices.networking.istio.io`
- `authorizationpolicies.security.istio.io`
- `peerauthentications.security.istio.io`

The documents are unchanged and retain their upstream order. The runner verifies SHA256 `c39b1abd1c3485809fa77441266f4ebb53d9305643c0e754db51fe0d1f11c354` before installation.
