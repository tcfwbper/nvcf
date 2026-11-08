#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

#
# Smoke tests for deploy/helm/nvsnap chart. Regression-net for issues like
# nvsnap#54 where the chart's rendered ClusterRole was missing rules for
# nvsnap.io/gpucheckpoints, causing /api/v1/checkpoints to 500 on the
# deployed server.
#
# Self-contained — needs only `helm` on PATH; no cluster connection.
# Exit 0 = all checks pass. Any failure exits non-zero.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CHART="$REPO_ROOT/deploy/helm/nvsnap"
RELEASE=nvsnap-test
NS=nvsnap-system

if ! command -v helm >/dev/null; then
  echo "FAIL: helm not on PATH" >&2
  exit 1
fi

fail() { echo "FAIL: $*" >&2; exit 1; }
pass() { echo "PASS: $*"; }

# NOTE: helm lint currently fails on this chart due to missing
# dependency declarations in Chart.yaml (jaeger, cert-manager,
# gpu-operator are referenced as subcharts but not declared as deps).
# Pre-existing on main; tracked separately. Skip lint until that's
# fixed so this regression test isn't held hostage by an unrelated bug.

# 1. helm template renders without error
RENDER=$(helm template "$RELEASE" "$CHART" --namespace "$NS") \
  || fail "helm template failed"
pass "helm template renders"

# The agents' ServiceMonitor selects exactly one Service, the headless
# nvsnap-agent: a selector matching none scrapes nothing, one matching the
# node-local Service too scrapes every agent twice.
SM_API=monitoring.coreos.com/v1/ServiceMonitor
SM_RENDER=$(helm template "$RELEASE" "$CHART" --namespace "$NS" \
  --api-versions "$SM_API" --set agent.hostNetwork=false) \
  || fail "helm template with the ServiceMonitor failed"
OBS_STACK="$REPO_ROOT/../../../deploy/stacks/observability/environments/base.yaml"
[[ -f "$OBS_STACK" ]] || fail "observability stack values not found at $OBS_STACK"
echo "$SM_RENDER" | python3 -c '
import sys, yaml
docs = [d for d in yaml.safe_load_all(sys.stdin) if d]
sm = [d for d in docs if d["kind"] == "ServiceMonitor" and d["metadata"]["name"] == "nvsnap-agent"]
if len(sm) != 1:
    sys.exit("want one nvsnap-agent ServiceMonitor, got %d" % len(sm))
# By default both collectors this repository knows pick it up: the cluster
# Prometheus agent (examples/cluster-monitoring-sample: namespace monitoring
# and its discover label) and the deploy/stacks/observability collector
# (its serviceMonitorSelector, read from the stack, any namespace).
if sm[0]["metadata"]["namespace"] != "monitoring":
    sys.exit("ServiceMonitor in namespace %s, want monitoring" % sm[0]["metadata"]["namespace"])
labels = sm[0]["metadata"]["labels"]
if labels.get("prometheus.agent/servicemonitor-discover") != "true":
    sys.exit("ServiceMonitor lacks the prometheus.agent/servicemonitor-discover label")
stack = yaml.safe_load(open(sys.argv[1]))
want = stack["collector"]["targetAllocator"]["prometheusCR"]["serviceMonitorSelector"]["matchLabels"]
if any(labels.get(k) != v for k, v in want.items()):
    sys.exit("ServiceMonitor labels %s do not match the observability collector selector %s" % (labels, want))
sel = sm[0]["spec"]["selector"]["matchLabels"]
port = sm[0]["spec"]["endpoints"][0]["port"]
hit = [d["metadata"]["name"] for d in docs if d["kind"] == "Service"
       and all((d["metadata"].get("labels") or {}).get(k) == v for k, v in sel.items())]
if hit != ["nvsnap-agent"]:
    sys.exit("ServiceMonitor selects Services %s, want [nvsnap-agent]" % hit)
svc = [d for d in docs if d["kind"] == "Service" and d["metadata"]["name"] == "nvsnap-agent"][0]
if port not in [p.get("name") for p in svc["spec"]["ports"]]:
    sys.exit("ServiceMonitor port %s is not a port of nvsnap-agent" % port)
cr = [d for d in docs if d["kind"] == "ClusterRole" and d["metadata"]["name"] == "nvsnap-agent"]
if not any("events" in r.get("resources", []) and "create" in r.get("verbs", []) for r in cr[0]["rules"]):
    sys.exit("the nvsnap-agent ClusterRole cannot create events")
' "$OBS_STACK" || fail "agent ServiceMonitor or its events rule is wrong"
# enabled: auto (the default) follows the cluster's API; true and false
# override it.
sm_count() { helm template "$RELEASE" "$CHART" --namespace "$NS" "$@" | grep -c '^kind: ServiceMonitor' || true; }
[[ "$(grep -c '^kind: ServiceMonitor' <<<"$RENDER" || true)" == 0 ]] \
  || fail "a ServiceMonitor renders on a cluster without the ServiceMonitor API"
[[ "$(sm_count --api-versions "$SM_API" --set agent.metrics.serviceMonitor.enabled=false)" == 0 ]] \
  || fail "enabled=false still renders a ServiceMonitor"
[[ "$(sm_count --set agent.metrics.serviceMonitor.enabled=true)" == 1 ]] \
  || fail "enabled=true does not render a ServiceMonitor without the API"
NOLABEL=$(helm template "$RELEASE" "$CHART" --namespace "$NS" --api-versions "$SM_API" \
  --set 'agent.metrics.serviceMonitor.labels.prometheus\.agent/servicemonitor-discover=null') \
  || fail "helm template with the default label dropped failed"
! grep -q "servicemonitor-discover" <<<"$NOLABEL" || fail "the default label cannot be dropped"
pass "agent ServiceMonitor selects the nvsnap-agent Service only, renders where the cluster serves the API, in monitoring with the discover label; the agent may create events"

# 2. ClusterRole 'nvsnap-server' has the nvsnap.io rule block (nvsnap#54)
# Split the rendered output on '---\n' (YAML doc separators), then
# pick the document that's both a ClusterRole AND named nvsnap-server.
CR_BLOCK=$(echo "$RENDER" | awk 'BEGIN{RS="\n---\n"} /kind: ClusterRole/ && /name: nvsnap-server/ && !n++ {print}')
[[ -n "$CR_BLOCK" ]] || fail "no ClusterRole named 'nvsnap-server' in rendered output"
echo "$CR_BLOCK" | grep -q 'gpucheckpoints' \
  || fail "nvsnap-server ClusterRole missing gpucheckpoints rule (nvsnap#54 regression)"
echo "$CR_BLOCK" | grep -q 'gpucheckpoints/status' \
  || fail "nvsnap-server ClusterRole missing gpucheckpoints/status rule"
echo "$CR_BLOCK" | grep -q 'gpurestores' \
  || fail "nvsnap-server ClusterRole missing gpurestores rule"
echo "$CR_BLOCK" | grep -q 'gpurestores/status' \
  || fail "nvsnap-server ClusterRole missing gpurestores/status rule"
pass "nvsnap-server ClusterRole has nvsnap.io rules"

# services:get — the observability proxy probes whether grafana /
# jaeger / prometheus Services exist before rendering UI tiles.
# Without this rule the probe Forbidden-fails silently and every
# tile shows "not installed" even when the subcharts are deployed.
# Regression test for the GCP-H100-a outage 2026-06-02.
echo "$CR_BLOCK" | awk '
  /resources:.*services/ { in_svc_block = 1 }
  in_svc_block && /verbs:/ { print; in_svc_block = 0 }
' | grep -q 'get' \
  || fail "nvsnap-server ClusterRole missing services:get rule (observability discovery)"
pass "nvsnap-server ClusterRole has services:get rule"

# storageclasses:get on the agent ClusterRole — the L2 backend
# probes the configured SC at startup. Without this rule the
# probe Forbidden-fails and L2 silently disables itself.
# Regression test for GCP-H100-a 2026-06-02 post-MR-39.
AGENT_CR_BLOCK=$(echo "$RENDER" | awk 'BEGIN{RS="\n---\n"} /kind: ClusterRole/ && /name: nvsnap-agent/ && !n++ {print}')
[[ -n "$AGENT_CR_BLOCK" ]] || fail "no ClusterRole named 'nvsnap-agent' in rendered output"
echo "$AGENT_CR_BLOCK" | grep -q 'storageclasses' \
  || fail "nvsnap-agent ClusterRole missing storageclasses:get rule (L2 startup probe)"
pass "nvsnap-agent ClusterRole has storageclasses:get rule"

# 3. CRD manifests live in chart/crds/ so Helm installs them on
# `helm install` (nvsnap#54: CRDs were absent from the chart entirely).
[[ -d "$CHART/crds" ]]                           || fail "chart/crds directory missing"
[[ -f "$CHART/crds/nvsnap.io_gpucheckpoints.yaml" ]] || fail "chart/crds/nvsnap.io_gpucheckpoints.yaml missing"

# CRD file must declare both gpucheckpoints + gpurestores
CRD_FILE="$CHART/crds/nvsnap.io_gpucheckpoints.yaml"
grep -q 'name: gpucheckpoints.nvsnap.io' "$CRD_FILE" \
  || fail "CRD file missing gpucheckpoints.nvsnap.io"
grep -q 'name: gpurestores.nvsnap.io' "$CRD_FILE" \
  || fail "CRD file missing gpurestores.nvsnap.io"
pass "chart ships CRDs for gpucheckpoints + gpurestores"

# 4. The chart's CRD file must match the canonical source in deploy/crds/
# — otherwise drift can ship a stale CRD via Helm while kubectl-apply
# users get the fresh one. Comments and blank lines are ignored: the
# chart copy carries the license header.
SRC_CRD="$REPO_ROOT/deploy/crds/nvsnap.io_gpucheckpoints.yaml"
# Comments and blank lines (the license header) may differ; the schema may not.
diff -q <(grep -v -E '^ *(#|$)' "$SRC_CRD") <(grep -v -E '^ *(#|$)' "$CRD_FILE") >/dev/null \
  || fail "chart/crds/nvsnap.io_gpucheckpoints.yaml drifted from deploy/crds/nvsnap.io_gpucheckpoints.yaml"
pass "chart CRD matches deploy/crds/ source"

# 4b. GPUCheckpoint CRD must declare every status field nvsnap-server writes,
# else K8s silently drops unknown fields and the hash never reaches the
# CRD — NVCA Hook B reads the empty hash, refuses to mark Warm, and
# retries forever (nvsnap#80, GCP-H100-a 2026-06-02). Same for the
# InProgress phase setCheckpointStatus emits at the start.
GPUCHK_BLOCK=$(awk '/kind: CustomResourceDefinition/{f=0} /name: gpucheckpoints/{f=1} f' "$CRD_FILE")
for field in 'checkpointHash:' 'checkpointPath:' 'checkpointSize:' 'nodeName:'; do
  echo "$GPUCHK_BLOCK" | grep -q "$field" \
    || fail "GPUCheckpoint status schema missing field $field (nvsnap-server writes it; K8s will silently drop)"
done
for phase in 'Pending' 'InProgress' 'Completed' 'Failed' 'Restoring' 'Restored'; do
  echo "$GPUCHK_BLOCK" | grep -q "    - $phase\$" \
    || fail "GPUCheckpoint phase enum missing value '$phase' (nvsnap-server emits it; K8s will reject the update)"
done
pass "GPUCheckpoint status schema covers every field nvsnap-server writes"

# 5. Every rendered nvsnap image ref must have a non-empty tag. Catches
# regressions like the v0.0.5 blobstore ImagePullBackOff (helm fell
# back to .Chart.AppVersion when the tag was empty in values.yaml,
# but that tag was never built+pushed). The chart now fails the
# render explicitly on empty tag — verify both the happy path here
# (all tags non-empty + correctly substituted) and the failure path.
while IFS= read -r ref; do
  case "$ref" in
    *:) fail "rendered image ref has empty tag: $ref" ;;
    nvcr.io/0651155215864979/ncp-dev/nvsnap-*:*)
      tag="${ref##*:}"
      [[ -n "$tag" ]] || fail "rendered image ref has empty tag after colon: $ref"
      ;;
  esac
done < <(echo "$RENDER" | grep -E '^\s*image:\s+nvcr\.io/0651155215864979/ncp-dev/nvsnap-' | sed -E 's/^\s*image:\s+//')
pass "every nvsnap image ref has a non-empty tag"

# 6. Mutation: wiping any image tag must fail the render with a
# clear error, not silently fall back to AppVersion. This is the
# regression test for the v0.0.5 blobstore ImagePullBackOff.
for img in agent server; do
  if helm template "$RELEASE" "$CHART" --namespace "$NS" --set "$img.image.tag=" >/dev/null 2>&1; then
    fail "empty $img.image.tag should fail the chart render but didn't"
  fi
done
pass "empty image tags fail the chart render with a clear error"

# 7. The server API gets no address outside the cluster: its Service is
# ClusterIP whatever server.service.type says, including a LoadBalancer
# that --reuse-values carries over from an older chart, and NOTES says the
# value is not read.
svc_type() {
  awk 'BEGIN{RS="\n---\n"} /kind: Service\n/ && /name: nvsnap-server\n/ {print; exit}' \
    | sed -n 's/^  type: //p'
}
[[ "$(svc_type <<<"$RENDER")" == ClusterIP ]] \
  || fail "nvsnap-server Service is not ClusterIP by default"
for t in LoadBalancer NodePort; do
  OUT=$(helm template "$RELEASE" "$CHART" --namespace "$NS" --set "server.service.type=$t") \
    || fail "the chart should render with a leftover server.service.type=$t"
  [[ "$(svc_type <<<"$OUT")" == ClusterIP ]] \
    || fail "server.service.type=$t made the nvsnap-server Service other than ClusterIP"
done
# NOTES.txt, rendered without a cluster: helm template does not print it,
# so a copy of the chart renders it into a ConfigMap.
notes_render() {
  local tmp out rc
  tmp=$(mktemp -d)
  cp -r "$CHART" "$tmp/chart"
  cp "$CHART/templates/NOTES.txt" "$tmp/chart/notes-render.txt"
  printf '%s\n' 'apiVersion: v1' 'kind: ConfigMap' 'metadata:' '  name: notes-render' 'data:' '  notes: |' \
    '{{- tpl (.Files.Get "notes-render.txt") . | nindent 4 }}' >"$tmp/chart/templates/notes-render.yaml"
  out=$(helm template "$RELEASE" "$tmp/chart" --namespace "$NS" --show-only templates/notes-render.yaml "$@" 2>&1)
  rc=$?
  rm -rf "$tmp"
  echo "$out"
  return $rc
}
NOTES=$(notes_render --set server.service.type=LoadBalancer) \
  || fail "NOTES does not render with server.service.type=LoadBalancer: $NOTES"
grep -q 'server.service.type=LoadBalancer is no longer read' <<<"$NOTES" \
  || fail "NOTES does not say a leftover server.service.type is not read"
grep -q 'nvsnap-server (REST API + UI, port 8080, type ClusterIP)' <<<"$NOTES" \
  || fail "NOTES does not give nvsnap-server's type as ClusterIP when server.service.type=LoadBalancer"
NOTES=$(notes_render) || fail "NOTES does not render: $NOTES"
grep -q 'nvsnap-server (REST API + UI, port 8080, type ClusterIP)' <<<"$NOTES" \
  || fail "NOTES does not give nvsnap-server's type as ClusterIP"
grep -q 'port-forward svc/nvsnap-server' <<<"$NOTES" \
  || fail "NOTES does not say how to reach nvsnap-server"
grep -q 'is no longer read' <<<"$NOTES" \
  && fail "NOTES warns about server.service.type when it is not set"
pass "nvsnap-server is ClusterIP whatever server.service.type says"

echo "OK — all helm chart checks passed"
