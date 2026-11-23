#!/usr/bin/env bash
set -euo pipefail

stack_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
work_dir="$(mktemp -d)"
trap 'rm -rf "$work_dir"' EXIT

fail() {
  echo "event-ledger-dependencies: $*" >&2
  exit 1
}

# Debug list renders the state without pulling charts or contacting Kubernetes.
HELMFILE_ENV=base HELMFILE_CACHE_HOME="$work_dir/helmfile-cache" helmfile \
  --file "$stack_dir/helmfile.d/02-core.yaml.gotmpl" \
  --log-level debug \
  --environment default \
  --state-values-set ingress.gatewayApi.controllerNamespace=envoy-gateway-system \
  --state-values-set ingress.gatewayApi.gateways.shared.name=shared-gw \
  --state-values-set ingress.gatewayApi.gateways.shared.namespace=envoy-gateway-system \
  --state-values-set ingress.gatewayApi.gateways.grpc.name=grpc-gw \
  --state-values-set ingress.gatewayApi.gateways.grpc.namespace=envoy-gateway-system \
  list --skip-charts --output json >"$work_dir/render.log" 2>&1 || {
    cat "$work_dir/render.log" >&2
    fail "Helmfile state did not render"
  }

awk '
  /^rendering result of / { print "---"; capture = 1; next }
  capture && /^ *[0-9]+: / { sub(/^ *[0-9]+: ?/, ""); print; next }
  capture { capture = 0 }
' "$work_dir/render.log" >"$work_dir/state.yaml"

needs="$(yq ea -r 'select(.releases) | .releases[] | select(.name == "event-ledger") | .needs[]' "$work_dir/state.yaml")"
test "$needs" = "api-keys/api-keys" ||
  fail "Event Ledger must wait for api-keys/api-keys, got $needs"

icms_needs="$(yq ea -r 'select(.releases) | .releases[] | select(.name == "sis") | .needs[]' "$work_dir/state.yaml")"
test "$icms_needs" = "api-keys/api-keys" ||
  fail "ICMS must wait for api-keys/api-keys, got $icms_needs"

provider="$(yq ea -r 'select(.releases) | .releases[] | select(.name == "api-keys") | .namespace + "/" + .name' "$work_dir/state.yaml")"
test "$provider" = "$needs" ||
  fail "API Keys dependency must be declared in the same Helmfile state"

echo "event-ledger-dependencies: all checks passed"
