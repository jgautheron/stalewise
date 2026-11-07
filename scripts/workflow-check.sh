#!/usr/bin/env bash
# Checks that the image workflow signs the index digest its own merge produced, never a re-read tag.
set -euo pipefail
workflow="$(cd "$(dirname "$0")/.." && pwd)/.github/workflows/image.yml"
fail() { echo "workflow-check: $*" >&2; exit 1; }

steps() { yq -r ".jobs.publish.steps[] | select(.id == \"$1\") | .run" "$workflow"; }
sign_digest="$(yq -r '.jobs.publish.steps[] | select(.name == "Sign image keylessly") | .env.DIGEST' "$workflow")"
[[ "$sign_digest" == '${{ steps.merge.outputs.digest }}' ]] || fail "signing subject is $sign_digest, not the merge output"
steps merge | grep -qF -- '--metadata-file' || fail "the merge does not record its own digest"
if yq -r '.jobs.publish.steps[].run // ""' "$workflow" | grep -qE 'imagetools inspect [^@]*:\$'; then
  fail "a step re-reads a mutable tag"
fi
echo "workflow-check: ok"
