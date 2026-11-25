#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

stack_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
repo_dir="$(cd "$stack_dir/../../.." && pwd)"
chart="$repo_dir/deploy/helm/vanity-gateway/helm-nvcf-vanity-gateway"
work_dir="$(mktemp -d)"
trap 'rm -rf "$work_dir"' EXIT

# Render against a disposable stack copy so the test needs no local secrets.
test_stack_dir="$work_dir/self-managed"
cp -R "$stack_dir" "$test_stack_dir"
printf '{}\n' >"$test_stack_dir/secrets/base-secrets.yaml"

assert_limit() {
  local manifest="$1" expected="$2"
  grep -q "^  MAX_REQUEST_SIZE_BYTES: \"$expected\"$" "$manifest" || {
    echo "expected MAX_REQUEST_SIZE_BYTES=$expected in $manifest" >&2
    exit 1
  }
}

assert_streamed_limit() {
  local manifest="$1" expected="$2"
  if [[ "$expected" == unset ]]; then
    if grep -q "MAX_STREAMED_REQUEST_SIZE_BYTES" "$manifest"; then
      echo "expected no MAX_STREAMED_REQUEST_SIZE_BYTES in $manifest" >&2
      exit 1
    fi
    return
  fi
  grep -q "^  MAX_STREAMED_REQUEST_SIZE_BYTES: \"$expected\"$" "$manifest" || {
    echo "expected MAX_STREAMED_REQUEST_SIZE_BYTES=$expected in $manifest" >&2
    exit 1
  }
}

# A chart released before the streamed limit, with the key absent from its
# schema, so stack values that leave the limit unset still render on rollback.
old_chart="$work_dir/old-chart"
cp -R "$chart" "$old_chart"
python3 - "$old_chart/values.schema.json" <<'PY'
import json, sys
path = sys.argv[1]
with open(path) as f:
    schema = json.load(f)
config = schema["properties"]["vanityGateway"]["properties"]["config"]["properties"]
del config["maxStreamedRequestSizeBytes"]
with open(path, "w") as f:
    json.dump(schema, f, indent=2)
PY
if helm template vanity-gateway "$old_chart" \
  --set "vanityGateway.config.maxStreamedRequestSizeBytes=67108864" >"$work_dir/old.log" 2>&1; then
  echo "older chart accepted the streamed limit key" >&2
  exit 1
fi

helm template vanity-gateway "$chart" >"$work_dir/default.yaml"
assert_limit "$work_dir/default.yaml" 26214400
assert_streamed_limit "$work_dir/default.yaml" unset
for limit in 1 67108864; do
  helm template vanity-gateway "$chart" \
    --set "vanityGateway.config.maxStreamedRequestSizeBytes=$limit" >"$work_dir/custom.yaml"
  assert_streamed_limit "$work_dir/custom.yaml" "$limit"
  assert_limit "$work_dir/custom.yaml" 26214400
done
helm template vanity-gateway "$chart" \
  --set "vanityGateway.config.maxStreamedRequestSizeBytes=null" >"$work_dir/custom.yaml"
assert_streamed_limit "$work_dir/custom.yaml" unset

for limit in 0 -1 1.5 nope 9223372036854775808; do
  if helm template vanity-gateway "$chart" \
    --set "vanityGateway.config.maxStreamedRequestSizeBytes=$limit" >"$work_dir/invalid.log" 2>&1; then
    echo "chart accepted invalid streamed request limit: $limit" >&2
    exit 1
  fi
  grep -q maxStreamedRequestSizeBytes "$work_dir/invalid.log"
done
for limit in 1 67108864; do
  helm template vanity-gateway "$chart" \
    --set "vanityGateway.config.maxRequestSizeBytes=$limit" >"$work_dir/custom.yaml"
  assert_limit "$work_dir/custom.yaml" "$limit"
done

for limit in 0 -1 1.5 nope null 9223372036854775808; do
  if helm template vanity-gateway "$chart" \
    --set "vanityGateway.config.maxRequestSizeBytes=$limit" >"$work_dir/invalid.log" 2>&1; then
    echo "chart accepted invalid request limit: $limit" >&2
    exit 1
  fi
  grep -q maxRequestSizeBytes "$work_dir/invalid.log"
done

for limit in default 67108864 0; do
  overrides=()
  expected=26214400
  if [[ "$limit" != default ]]; then
    overrides+=(--state-values-set "addons.vanityGateway.config.maxRequestSizeBytes=$limit")
    expected="$limit"
  fi
  (
    cd "$test_stack_dir"
    HELMFILE_ENV=base HELMFILE_CACHE_HOME="$work_dir/helmfile-cache" helmfile \
      --file helmfile.d/02-core.yaml.gotmpl --environment default \
      --state-values-set addons.vanityGateway.enabled=true \
      --state-values-set ingress.gatewayApi.gateways.shared.name=shared-gw \
      --state-values-set ingress.gatewayApi.gateways.shared.namespace=envoy-gateway-system \
      --state-values-set ingress.gatewayApi.gateways.grpc.name=grpc-gw \
      --state-values-set ingress.gatewayApi.gateways.grpc.namespace=envoy-gateway-system \
      ${overrides[@]+"${overrides[@]}"} --selector name=vanity-gateway \
      write-values --output-file-template "$work_dir/stack-values.yaml" >/dev/null
  )
  if [[ "$limit" == 0 ]]; then
    if helm template vanity-gateway "$chart" -f "$work_dir/stack-values.yaml" >"$work_dir/invalid.log" 2>&1; then
      echo "stack replaced explicit zero with a default" >&2
      exit 1
    fi
    grep -q maxRequestSizeBytes "$work_dir/invalid.log"
  else
    helm template vanity-gateway "$chart" -f "$work_dir/stack-values.yaml" >"$work_dir/stack.yaml"
    assert_limit "$work_dir/stack.yaml" "$expected"
  fi
done

for limit in default 67108864 0; do
  overrides=()
  if [[ "$limit" != default ]]; then
    overrides+=(--state-values-set "addons.vanityGateway.config.maxStreamedRequestSizeBytes=$limit")
  fi
  (
    cd "$test_stack_dir"
    HELMFILE_ENV=base HELMFILE_CACHE_HOME="$work_dir/helmfile-cache" helmfile \
      --file helmfile.d/02-core.yaml.gotmpl --environment default \
      --state-values-set addons.vanityGateway.enabled=true \
      --state-values-set ingress.gatewayApi.gateways.shared.name=shared-gw \
      --state-values-set ingress.gatewayApi.gateways.shared.namespace=envoy-gateway-system \
      --state-values-set ingress.gatewayApi.gateways.grpc.name=grpc-gw \
      --state-values-set ingress.gatewayApi.gateways.grpc.namespace=envoy-gateway-system \
      ${overrides[@]+"${overrides[@]}"} --selector name=vanity-gateway \
      write-values --output-file-template "$work_dir/stack-values.yaml" >/dev/null
  )
  case "$limit" in
    default)
      helm template vanity-gateway "$chart" -f "$work_dir/stack-values.yaml" >"$work_dir/stack.yaml"
      assert_streamed_limit "$work_dir/stack.yaml" unset
      helm template vanity-gateway "$old_chart" -f "$work_dir/stack-values.yaml" >"$work_dir/stack-old.yaml"
      assert_limit "$work_dir/stack-old.yaml" 26214400
      ;;
    0)
      if helm template vanity-gateway "$chart" -f "$work_dir/stack-values.yaml" >"$work_dir/invalid.log" 2>&1; then
        echo "stack dropped an explicit zero streamed limit" >&2
        exit 1
      fi
      grep -q maxStreamedRequestSizeBytes "$work_dir/invalid.log"
      ;;
    *)
      helm template vanity-gateway "$chart" -f "$work_dir/stack-values.yaml" >"$work_dir/stack.yaml"
      assert_streamed_limit "$work_dir/stack.yaml" "$limit"
      ;;
  esac
done

echo "vanity-request-size: all checks passed"
