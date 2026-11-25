<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# cmd/nvsnap-mount-prep

`nvsnap-mount-prep`, an init container the webhook adds to a restore pod
when restore mounts are prepared outside admission. It asks the node agent
to prepare the pod's OverlayFS restore mounts (`POST /v1/restore/prep`), then
polls `GET /v1/restore/prep/{pod-uid}` until the job is ready or failed.

## Used by

- [`internal/webhook/mount_prep_init.go`](../../internal/webhook/mount_prep_init.go),
  when the chart sets `webhook.restorePrepStrategy: init-container`
  (default `inline`). Its image is `webhook.mountPrepInitImage`, which
  defaults to the agent image.
- Served by the handlers in `restore_prep_http.go` of
  [`internal/agent`](../../internal/agent/), routed in `agent.go`.
- Built into the agent image by
  [`docker/agent/Dockerfile.app`](../../docker/agent/Dockerfile.app).
- Entry point: `main.go:main`.

## Key files

- `main.go`: the whole program. Its package comment lists the environment
  it reads (`NVSNAP_POD_UID`, `NVSNAP_RESTORE_HASH`, `NVSNAP_AGENT_URL`,
  `NVSNAP_PREP_MOUNTS`, ...) and its exit codes.
