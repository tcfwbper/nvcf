<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# cmd/nvsnap-server

`nvsnap-server`, the cluster-wide API server Deployment. It opens the SQLite
catalog ([`internal/db`](../../internal/db/)), builds the REST API
([`internal/server`](../../internal/server/)), serves the embedded web UI
([`ui/`](../../ui/); the `docker/Dockerfile.server` image builds it in,
the Bazel image does not) and starts OpenTelemetry tracing
([`internal/tracing`](../../internal/tracing/)).

## Used by

- The `server.yaml` template of [`deploy/helm/nvsnap`](../../deploy/helm/nvsnap/)
  (`server.enabled`, default `true`).
- Built by [`docker/Dockerfile.server`](../../docker/Dockerfile.server), the
  Makefile `nvsnap-server` target and the root `BUILD.bazel`
  `nvsnap_server_image`.
- Entry point: `main.go:main`, which runs `main.go:run`.

## Key files

- `main.go`: flags (`--address`, `--agent-port`, `--db-path`,
  `--kubeconfig`, `--log-level`), the agent token from
  `NVSNAP_AGENT_TOKEN`, and server start-up and shutdown.
