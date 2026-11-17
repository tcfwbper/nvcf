#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Test that api.accountBootstrap.enabled set in environments/<env>.yaml reaches
# the API chart, so an install whose account already exists can skip the
# account-bootstrap hook Job. Unset, the chart default applies.
set -euo pipefail

stack_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
repo_dir="$(cd "$stack_dir/../../.." && pwd)"
work_dir="$(mktemp -d)"
test_stack_dir="$work_dir/self-managed"
environment_name="account-bootstrap-wiring-test"
environment_file="$test_stack_dir/environments/$environment_name.yaml"
secrets_file="$test_stack_dir/secrets/$environment_name-secrets.yaml"
trap 'rm -rf "$work_dir"' EXIT

fail() {
  echo "account-bootstrap-wiring: $*" >&2
  exit 1
}

mkdir -p "$test_stack_dir"
cp -R "$stack_dir"/. "$test_stack_dir"
printf '{}\n' >"$secrets_file"

write_environment() {
  cat >"$environment_file"
}

render_api_values() {
  local output_file="$1"

  HELMFILE_ENV="$environment_name" \
    HELMFILE_CACHE_HOME="$work_dir/helmfile-cache" \
    helmfile \
      --file "$test_stack_dir/helmfile.d/02-core.yaml.gotmpl" \
      --environment default \
      --state-values-set ingress.gatewayApi.controllerNamespace=envoy-gateway-system \
      --state-values-set ingress.gatewayApi.gateways.shared.name=shared-gw \
      --state-values-set ingress.gatewayApi.gateways.shared.namespace=envoy-gateway-system \
      --state-values-set ingress.gatewayApi.gateways.grpc.name=grpc-gw \
      --state-values-set ingress.gatewayApi.gateways.grpc.namespace=envoy-gateway-system \
      --selector name=api \
      write-values \
      --output-file-template "$output_file"
}

# Prints the number of account-bootstrap hook Jobs the API chart renders.
count_bootstrap_jobs() {
  local values_file="$1"

  helm template api "$repo_dir/deploy/helm/cloud-functions/nvcf-api" \
    --namespace nvcf \
    --values "$values_file" |
    yq ea '[select(.kind == "Job" and .metadata.labels."app.kubernetes.io/component" == "account-bootstrap")] | length'
}

# Unset: the stack renders no enabled key and the chart default runs the Job.
write_environment <<'EOF'
global:
  image:
    registry: nvcr.io
    repository: nvidia/nvcf
EOF
render_api_values "$work_dir/default.yaml" >/dev/null ||
  fail "default environment did not render"
[[ "$(yq -r '.api.accountBootstrap | has("enabled")' "$work_dir/default.yaml")" == false ]] ||
  fail "unset api.accountBootstrap.enabled must stay chart-owned"
[[ "$(count_bootstrap_jobs "$work_dir/default.yaml")" == 1 ]] ||
  fail "chart default must render the account-bootstrap Job"

# false: the API chart renders no account-bootstrap Job.
write_environment <<'EOF'
global:
  image:
    registry: nvcr.io
    repository: nvidia/nvcf
api:
  accountBootstrap:
    enabled: false
EOF
render_api_values "$work_dir/disabled.yaml" >/dev/null ||
  fail "disabled environment did not render"
[[ "$(yq -r '.api.accountBootstrap.enabled' "$work_dir/disabled.yaml")" == false ]] ||
  fail "api.accountBootstrap.enabled: false did not reach the API values"
[[ "$(count_bootstrap_jobs "$work_dir/disabled.yaml")" == 0 ]] ||
  fail "api.accountBootstrap.enabled: false still renders the account-bootstrap Job"

# Only a boolean passes. Null would render as "<no value>", which the chart
# reads as true, so the Job would still run. Quoted values are rejected too.
for value in null '"false"'; do
  write_environment <<EOF
global:
  image:
    registry: nvcr.io
    repository: nvidia/nvcf
api:
  accountBootstrap:
    enabled: ${value}
EOF
  if render_api_values "$work_dir/invalid.yaml" >"$work_dir/invalid.log" 2>&1; then
    fail "api.accountBootstrap.enabled: ${value} was accepted"
  fi
  grep -Fq "api.accountBootstrap.enabled must be a YAML boolean" "$work_dir/invalid.log" ||
    fail "api.accountBootstrap.enabled: ${value} did not return the expected error"
done

echo "account-bootstrap-wiring: ok"
