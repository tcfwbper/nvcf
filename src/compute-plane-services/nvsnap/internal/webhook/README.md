<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# internal/webhook

The mutating admission webhook. It runs inside the agent process and turns
each admitted pod into JSON Patch operations: restore mounts and init
containers for a pod that restores a capture, the cachedir environment for
a pod that may be captured, CRIU restore placeholders, gpushare set-up, and
shared model volumes or node model cache steps.

## Used by

- [`internal/agent/webhook_integration.go`](../agent/webhook_integration.go)
  builds the `Mutator` and `Handler` and starts the HTTPS server
  (`NewServer`, `Run`).
- [`internal/agent`](../agent/) also uses its annotation names and helpers
  (for example `CRIURestoreAnnotation`, `PodBundleID`, `GroupUnusedVolumes`).
- The API server calls it through the `MutatingWebhookConfiguration` in the
  chart's `webhook.yaml` (path `/mutate`).

## Key files

- `server.go`, `admission.go`, `cert.go`: HTTPS server, AdmissionReview
  handling, TLS.
- `mutate.go`: `Mutator.Mutate`, which dispatches by pod and capture kind.
- `cachedir.go`, `rootfs_l2_overlay.go`, `l2_mount.go`: cachedir and rootfs
  restore mounts.
- `criu_restore.go`, `criu_group.go`: CRIU restore placeholders and groups.
- `gpushare.go`: gpushare for pods annotated `nvsnap.io/gpushare=true`
  and, when `GPUShareByDefault` is set (the agent sets it when
  `NVSNAP_DEFAULT_CAPTURE_PATH` is `criu`),
  for GPU pods that do not run to completion; `nvsnap.io/gpushare=false`
  opts out.
- `model_volume.go`, `node_model_cache.go`, `election.go`: model sharing.
- `capture_hold.go`, `helper.go`, `bundle_mount.go`: the readiness gate,
  `nvsnap-helper` steps and the node bundle mount.

## Configuration

Helm `webhook.enabled` (default `true`, the agent's `--webhook`),
`webhook.inject` (default `true`, the registration), `webhook.injectScope`
(default `all`), `webhook.failurePolicy` (default `Ignore`). Most other
`webhook.*` values become `--webhook-*` agent flags; some only shape chart
objects (`serviceName`, `cert`, `agentHostCIDR`,
`ephemeralContainerPolicy`) or an environment variable (`debugEnv`).

See [docs/ARCHITECTURE.md](../../docs/ARCHITECTURE.md).
